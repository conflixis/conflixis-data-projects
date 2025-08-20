/**
 * Corporate Structure Resolution Tool
 *
 * This module provides AI-powered tools to understand and resolve corporate structures:
 * - Identify subsidiary and parent company relationships
 * - Detect division and business unit names
 * - Handle common corporate structure patterns
 * - Suggest parent companies when subsidiaries don't match
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

// Common patterns that indicate subsidiary relationships
export const SUBSIDIARY_PATTERNS = [
  // Division patterns
  /(.+)\s+division\s*$/i,
  /(.+)\s+div\.?\s*$/i,

  // Business unit patterns
  /(.+)\s+business\s+unit\s*$/i,
  /(.+)\s+BU\s*$/i,

  // Geographic subsidiaries
  /(.+)\s+\((USA|UK|EU|Asia|EMEA|APAC|Americas)\)\s*$/i,
  /(.+)\s+(USA|UK|EU|Asia|EMEA|APAC|Americas)\s*$/i,

  // Industry subsidiaries
  /(.+)\s+(Healthcare|Financial|Technology|Services|Solutions)\s*$/i,

  // Ownership patterns
  /(.+),?\s+a\s+(.+)\s+(company|subsidiary|division)\s*$/i,
  /(.+)\s+by\s+(.+)\s*$/i,

  // Product/Brand subsidiaries
  /(.+)\s+powered\s+by\s+(.+)\s*$/i,
  /(.+)\s+from\s+(.+)\s*$/i,
];

// Common corporate structure indicators
export const STRUCTURE_INDICATORS = {
  subsidiary: [
    'subsidiary',
    'division',
    'unit',
    'branch',
    'affiliate',
    'owned by',
    'part of',
    'a division of',
    'a subsidiary of',
    'group company',
    'portfolio company',
  ],
  parent: [
    'parent',
    'holding',
    'group',
    'holdings',
    'corporation',
    'enterprises',
    'worldwide',
    'global',
    'international',
  ],
  regional: [
    'USA',
    'Americas',
    'EMEA',
    'APAC',
    'Asia',
    'Europe',
    'North America',
    'South America',
    'Pacific',
    'Atlantic',
  ],
  business: [
    'healthcare',
    'financial',
    'technology',
    'services',
    'solutions',
    'systems',
    'products',
    'digital',
    'cloud',
  ],
};

/**
 * Extract potential parent company name from subsidiary patterns
 */
export function extractParentFromPattern(companyName: string): string | null {
  for (const pattern of SUBSIDIARY_PATTERNS) {
    const match = companyName.match(pattern);
    if (match) {
      // Return the captured parent company name
      return match[1]?.trim() || match[2]?.trim() || null;
    }
  }

  // Check for explicit parent mentions
  const parentMatch = companyName.match(/(.+?)\s+(?:subsidiary|division|unit)/i);
  if (parentMatch) {
    return parentMatch[1].trim();
  }

  return null;
}

/**
 * Detect if a company name appears to be a subsidiary
 */
export function detectSubsidiaryIndicators(companyName: string): {
  isLikelySubsidiary: boolean;
  indicators: string[];
  confidence: number;
} {
  const lowerName = companyName.toLowerCase();
  const indicators: string[] = [];

  // Check for subsidiary keywords
  for (const keyword of STRUCTURE_INDICATORS.subsidiary) {
    if (lowerName.includes(keyword)) {
      indicators.push(keyword);
    }
  }

  // Check for regional indicators
  for (const region of STRUCTURE_INDICATORS.regional) {
    if (lowerName.includes(region.toLowerCase())) {
      indicators.push(`regional: ${region}`);
    }
  }

  // Check for business unit indicators
  for (const business of STRUCTURE_INDICATORS.business) {
    if (lowerName.endsWith(` ${business}`) || lowerName.includes(` ${business} `)) {
      indicators.push(`business unit: ${business}`);
    }
  }

  // Check for pattern matches
  for (const pattern of SUBSIDIARY_PATTERNS) {
    if (pattern.test(companyName)) {
      indicators.push('matches subsidiary pattern');
      break;
    }
  }

  const confidence = Math.min(indicators.length * 0.3, 1.0);

  return {
    isLikelySubsidiary: indicators.length > 0,
    indicators,
    confidence,
  };
}

// AI Tool Schemas
const identifyParentCompanySchema = z.object({
  hasParent: z.boolean(),
  parentCompanyName: z.string().optional(),
  confidence: z.number().min(0).max(1),
  relationshipType: z.enum([
    'subsidiary',
    'division',
    'business_unit',
    'brand',
    'regional_office',
    'unknown',
  ]),
  reasoning: z.string(),
  searchSuggestions: z.array(z.string()).optional(),
});

const resolveCorporateStructureSchema = z.object({
  structure: z.object({
    type: z.enum(['standalone', 'subsidiary', 'parent', 'conglomerate']),
    parentCompany: z.string().optional(),
    knownSubsidiaries: z.array(z.string()).optional(),
    businessUnits: z.array(z.string()).optional(),
  }),
  confidence: z.number().min(0).max(1),
  reasoning: z.string(),
  alternativeNames: z.array(z.string()),
});

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

/**
 * AI tool to identify parent company relationships
 */
export async function identifyParentCompany(
  params: IdentifyParentCompanyParams,
  model?: string
): Promise<IdentifyParentCompanyResult> {
  try {
    const prompt = `
You are an expert in corporate structures and company relationships.

Analyze this company name to determine if it's a subsidiary, division, or part of a larger organization:
Company Name: "${params.companyName}"

${
  params.context
    ? `
Context:
${params.context.industry ? `- Industry: ${params.context.industry}` : ''}
${params.context.region ? `- Region: ${params.context.region}` : ''}
${params.context.description ? `- Description: ${params.context.description}` : ''}
`
    : ''
}

Consider:
1. Common subsidiary patterns (e.g., "X Division", "Y Healthcare", "Z Americas")
2. Well-known corporate structures (e.g., Alphabet/Google, Meta/Facebook)
3. Regional offices or branches
4. Business unit naming conventions
5. Industry-specific patterns

Provide:
- hasParent: Whether this appears to be part of a larger organization
- parentCompanyName: The likely parent company name (if applicable)
- confidence: How confident you are (0.0-1.0)
- relationshipType: The type of relationship
- reasoning: Clear explanation of your analysis
- searchSuggestions: Alternative names to search for (parent company, common variations)

Examples:
- "Google Cloud" → Parent: "Google" (or "Alphabet")
- "Microsoft Azure" → Parent: "Microsoft"
- "GE Healthcare" → Parent: "General Electric"
- "Amazon Web Services" → Parent: "Amazon"
- "IBM Watson" → Parent: "IBM"`;

    const result = await generateObject({
      model: getModel(model),
      schema: identifyParentCompanySchema,
      system:
        'You are a corporate structure expert. Always provide accurate analysis based on real corporate relationships.',
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof identifyParentCompanySchema>;
    const usage = result.usage;

    return { ...object, tokenUsage: convertUsage(usage) };
  } catch (error) {
    console.error('Error in identifyParentCompany:', error);
    return {
      hasParent: false,
      confidence: 0,
      relationshipType: 'unknown',
      reasoning: 'Unable to analyze due to AI service error',
    };
  }
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

/**
 * AI tool to resolve complex corporate structures
 */
export async function resolveCorporateStructure(
  params: ResolveCorporateStructureParams,
  model?: string
): Promise<ResolveCorporateStructureResult> {
  try {
    const prompt = `
You are an expert in corporate structures and company hierarchies.

Analyze this company and provide information about its corporate structure:
Company Name: "${params.companyName}"

${
  params.failedMatches
    ? `
Previous failed searches: ${params.failedMatches.join(', ')}
`
    : ''
}

${
  params.context
    ? `
Context:
${params.context.industry ? `- Industry: ${params.context.industry}` : ''}
${params.context.size ? `- Company Size: ${params.context.size}` : ''}
${params.context.description ? `- Description: ${params.context.description}` : ''}
`
    : ''
}

Determine:
1. The corporate structure type (standalone, subsidiary, parent company, conglomerate)
2. Parent company name if it's a subsidiary
3. Known subsidiaries if it's a parent company
4. Major business units or divisions
5. Alternative names this company might be known by

Consider recent mergers, acquisitions, spin-offs, and rebrandings.

Provide alternative names including:
- Parent company name (if subsidiary)
- Common abbreviations
- Previous names
- DBA (Doing Business As) names
- International variations`;

    const result = await generateObject({
      model: getModel(model),
      schema: resolveCorporateStructureSchema,
      system:
        'You are a corporate structure expert with knowledge of company hierarchies, mergers, and acquisitions.',
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof resolveCorporateStructureSchema>;
    const usage = result.usage;

    return { ...object, tokenUsage: convertUsage(usage) };
  } catch (error) {
    console.error('Error in resolveCorporateStructure:', error);
    return {
      structure: {
        type: 'standalone',
      },
      confidence: 0,
      reasoning: 'Unable to analyze due to AI service error',
      alternativeNames: [],
    };
  }
}

/**
 * Generate corporate structure variations
 */
export function generateCorporateVariations(companyName: string): string[] {
  const variations = new Set<string>();

  // Remove common subsidiary indicators to find parent
  const parentFromPattern = extractParentFromPattern(companyName);
  if (parentFromPattern) {
    variations.add(parentFromPattern);
  }

  // Try removing regional indicators
  for (const region of STRUCTURE_INDICATORS.regional) {
    const pattern = new RegExp(`\\s*\\(?${region}\\)?\\s*`, 'gi');
    const cleaned = companyName.replace(pattern, ' ').trim();
    if (cleaned !== companyName && cleaned.length > 3) {
      variations.add(cleaned);
    }
  }

  // Try removing business unit indicators
  for (const business of STRUCTURE_INDICATORS.business) {
    const pattern = new RegExp(`\\s+${business}\\s*$`, 'i');
    const cleaned = companyName.replace(pattern, '').trim();
    if (cleaned !== companyName && cleaned.length > 3) {
      variations.add(cleaned);
    }
  }

  // Add common parent company suffixes
  const baseName = companyName.replace(/\s+(inc|corp|ltd|llc|co)\.?$/i, '').trim();
  if (baseName !== companyName) {
    variations.add(baseName);
    variations.add(`${baseName} Group`);
    variations.add(`${baseName} Holdings`);
    variations.add(`${baseName} Corporation`);
  }

  // Handle "X by Y" or "X from Y" patterns
  const byPattern = companyName.match(/(.+?)\s+(?:by|from)\s+(.+)/i);
  if (byPattern) {
    variations.add(byPattern[1].trim());
    variations.add(byPattern[2].trim());
  }

  return Array.from(variations).filter(v => v !== companyName && v.length > 2);
}

/**
 * Main interface for corporate structure detection
 */
export interface CorporateStructureDetectionResult {
  isSubsidiary: boolean;
  parentCompanySuggestions: string[];
  confidence: number;
  indicators: string[];
  variations: string[];
}

/**
 * Detect corporate structure and suggest parent companies
 */
export function detectCorporateStructure(companyName: string): CorporateStructureDetectionResult {
  const { isLikelySubsidiary, indicators, confidence } = detectSubsidiaryIndicators(companyName);
  const variations = generateCorporateVariations(companyName);

  const parentSuggestions: string[] = [];

  // Extract parent from patterns
  const parentFromPattern = extractParentFromPattern(companyName);
  if (parentFromPattern) {
    parentSuggestions.push(parentFromPattern);
  }

  // Add variations that could be parent companies
  for (const variation of variations) {
    if (!parentSuggestions.includes(variation)) {
      parentSuggestions.push(variation);
    }
  }

  return {
    isSubsidiary: isLikelySubsidiary,
    parentCompanySuggestions: parentSuggestions,
    confidence,
    indicators,
    variations,
  };
}
