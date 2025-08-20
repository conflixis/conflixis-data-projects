import { generateObject } from 'ai';
import { z } from 'zod';
import { getModel } from '../config/ai';
import { config } from '../config/environment';
import {
  CheckAbbreviationMatchParams,
  CheckAbbreviationMatchResult,
  DisambiguateMultipleMatchesParams,
  DisambiguateMultipleMatchesResult,
  EvaluateCompanyMatchParams,
  EvaluateCompanyMatchResult,
  GuessCompanyFromAbbreviationParams,
  GuessCompanyFromAbbreviationResult,
  TokenUsage,
} from '../types';

// Helper function to convert AI SDK usage to our TokenUsage type
function convertUsage(usage: unknown): TokenUsage | undefined {
  if (!usage || typeof usage !== 'object') return undefined;

  const usageObj = usage as Record<string, unknown>;

  // Extract token counts from the usage object
  const promptTokens = Number(usageObj.promptTokens || usageObj.inputTokens || 0);
  const completionTokens = Number(usageObj.completionTokens || usageObj.outputTokens || 0);
  const totalTokens = Number(usageObj.totalTokens || promptTokens + completionTokens || 0);

  return {
    promptTokens,
    completionTokens,
    totalTokens,
  };
}

// Schema definitions for AI responses
const evaluateCompanyMatchSchema = z.object({
  confidence: z.number().min(0).max(1),
  reasoning: z.string(),
  matchType: z.enum(['exact', 'abbreviation', 'variation', 'unlikely']),
  suggestedCompanyName: z.string().optional(),
});

const checkAbbreviationMatchSchema = z.object({
  isValidAbbreviation: z.boolean(),
  confidence: z.number().min(0).max(1),
  commonUsage: z.boolean(),
  suggestedCompanyName: z.string().optional(),
});

const disambiguateMultipleMatchesSchema = z.object({
  bestMatchIndex: z.number().int().min(0),
  confidence: z.number().min(0).max(1),
  reasoning: z.string(),
});

const guessCompanyFromAbbreviationSchema = z.object({
  possibleCompanies: z.array(
    z.object({
      name: z.string(),
      confidence: z.number().min(0).max(1),
      reasoning: z.string(),
    })
  ),
});

/**
 * Evaluate if a candidate company name matches a potential company
 */
export async function evaluateCompanyMatch(
  params: EvaluateCompanyMatchParams,
  model?: string
): Promise<EvaluateCompanyMatchResult & { tokenUsage?: TokenUsage }> {
  try {
    const prompt = `
You are a company matching expert. Evaluate if the candidate company name matches the potential company.

Candidate Name: ${JSON.stringify(params.candidateName)}

Potential Match:
- Name: ${JSON.stringify(params.potentialMatch.name)}

${
  params.context
    ? `
User Context:
${params.context.userIndustryHint ? `- Industry Hint: ${params.context.userIndustryHint}` : ''}
${params.context.geographicRegion ? `- Geographic Region: ${params.context.geographicRegion}` : ''}
${params.context.sizeCategory ? `- Size Category: ${params.context.sizeCategory}` : ''}
`
    : ''
}

Evaluate the match and provide:
1. A confidence score (0.0-1.0) where 1.0 is certain match
2. Clear reasoning for your evaluation
3. Match type classification:
   - "exact": Names are identical or near-identical
   - "abbreviation": One is a clear abbreviation of the other
   - "variation": Names are variations of the same company
   - "unlikely": Names are probably different companies

Consider factors like:
- Common abbreviations and variations
- Industry context relevance
- Domain name correlation
- Known aliases
- Regional naming conventions

Respond with a JSON object containing: confidence (number), reasoning (string), matchType (string), 
and optionally suggestedCompanyName (string) if you know a better search term.

IMPORTANT: If the candidate seems to be an abbreviation or variation of a well-known company 
(e.g., "J&J" for "Johnson & Johnson"), include the full company name in suggestedCompanyName.`;

    const result = await generateObject({
      model: getModel(model),
      schema: evaluateCompanyMatchSchema,
      system:
        'You are a company matching expert. Always respond with valid JSON only, no additional text.',
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof evaluateCompanyMatchSchema>;
    const usage = result.usage;

    // Add token usage if available
    const tokenUsage = convertUsage(usage);

    return { ...object, tokenUsage };
  } catch (error) {
    console.error('Error in evaluateCompanyMatch:', error);
    if (error instanceof Error) {
      console.error('Error details:', error.message);
      console.error('Stack trace:', error.stack);
    }
    // Return a conservative default on error
    return {
      confidence: 0.5,
      reasoning: 'Unable to evaluate match due to AI service error',
      matchType: 'unlikely' as const,
    };
  }
}

/**
 * Check if a short name is a valid abbreviation of a longer company name
 */
export async function checkAbbreviationMatch(
  params: CheckAbbreviationMatchParams,
  model?: string
): Promise<CheckAbbreviationMatchResult & { tokenUsage?: TokenUsage }> {
  try {
    const prompt = `
You are an expert in company abbreviations and naming conventions.

Determine if "${params.abbreviation}" is a valid abbreviation of "${params.fullName}".
${params.industryContext ? `Industry context: ${params.industryContext}` : ''}

Consider:
1. Is this a standard way to abbreviate this company name?
2. Is this abbreviation commonly used in the industry?
3. Would people in this industry recognize this abbreviation?

Examples of valid abbreviations:
- IBM for International Business Machines
- GE for General Electric
- P&G for Procter & Gamble
- MS for Microsoft
- JPMC for JPMorgan Chase

Provide a JSON response with:
- isValidAbbreviation: true/false
- confidence: 0.0-1.0
- commonUsage: whether this is a widely recognized abbreviation
- suggestedCompanyName: the full company name if you recognize the abbreviation (e.g., "Johnson & Johnson" for "J&J")`;

    const result = await generateObject({
      model: getModel(model),
      schema: checkAbbreviationMatchSchema,
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof checkAbbreviationMatchSchema>;
    const usage = result.usage;

    // Add token usage if available
    const tokenUsage = convertUsage(usage);

    return { ...object, tokenUsage };
  } catch (error) {
    console.error('Error in checkAbbreviationMatch:', error);
    return {
      isValidAbbreviation: false,
      confidence: 0.5,
      commonUsage: false,
    };
  }
}

/**
 * Choose the best match when multiple companies have similar names
 */
export async function disambiguateMultipleMatches(
  params: DisambiguateMultipleMatchesParams,
  model?: string
): Promise<DisambiguateMultipleMatchesResult & { tokenUsage?: TokenUsage }> {
  try {
    const prompt = `
You are an expert at disambiguating between similar company names.

The user is looking for: ${JSON.stringify(params.candidateName)}
${params.userContext ? `\nUser context: ${JSON.stringify(params.userContext)}` : ''}

Here are the potential matches (with confidence scores):
${params.competingMatches
  .map(
    (match, index) => `
${index}. ${JSON.stringify(match.name)}
   - Current confidence: ${match.confidenceScore.toFixed(2)}
`
  )
  .join('')}

Select the best match by:
1. Considering the user's intent and context
2. Evaluating industry relevance
3. Checking for exact matches or known aliases
4. Considering the most likely company the user is referring to

Provide a JSON response with:
- bestMatchIndex: The index (0-based) of the best match
- confidence: Your confidence in this selection (0.0-1.0)
- reasoning: Clear explanation of why this is the best match`;

    const result = await generateObject({
      model: getModel(model),
      schema: disambiguateMultipleMatchesSchema,
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof disambiguateMultipleMatchesSchema>;
    const usage = result.usage;

    // Validate index is within bounds
    if (object.bestMatchIndex >= params.competingMatches.length) {
      object.bestMatchIndex = 0;
    }

    // Add token usage if available
    const tokenUsage = convertUsage(usage);

    return { ...object, tokenUsage };
  } catch (error) {
    console.error('Error in disambiguateMultipleMatches:', error);
    // Default to the highest scoring match
    return {
      bestMatchIndex: 0,
      confidence: 0.5,
      reasoning: 'Unable to disambiguate due to AI service error',
    };
  }
}

/**
 * Guess what company an abbreviation might represent when no matches are found
 */
export async function guessCompanyFromAbbreviation(
  params: GuessCompanyFromAbbreviationParams,
  model?: string
): Promise<GuessCompanyFromAbbreviationResult & { tokenUsage?: TokenUsage }> {
  try {
    const prompt = `
You are an expert at recognizing company abbreviations and acronyms.

The user searched for "${params.abbreviation}" but no matches were found in the database.
${params.context?.industry ? `Industry context: ${params.context.industry}` : ''}
${params.context?.region ? `Geographic region: ${params.context.region}` : ''}

Based on the abbreviation pattern, suggest what companies this might refer to.
Consider:
1. Common company abbreviations (e.g., GE = General Electric, IBM = International Business Machines)
2. Stock tickers that match company names
3. Industry-specific abbreviations
4. Regional variations
5. Recently formed companies or spin-offs (e.g., GEHC = GE HealthCare after spinning off from General Electric)

Provide up to 3 possible company names that this abbreviation might represent, 
ordered by likelihood. For each suggestion, include:
- The full company name
- Confidence score (0.0-1.0)
- Brief reasoning

Focus on real companies that are likely to be in a business database.`;

    const result = await generateObject({
      model: getModel(model),
      schema: guessCompanyFromAbbreviationSchema,
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof guessCompanyFromAbbreviationSchema>;
    const usage = result.usage;

    // Add token usage if available
    const tokenUsage = convertUsage(usage);

    return { ...object, tokenUsage };
  } catch (error) {
    console.error('Error in guessCompanyFromAbbreviation:', error);
    return {
      possibleCompanies: [],
    };
  }
}

/**
 * Batch evaluate multiple low-confidence matches
 */
export async function batchEvaluateMatches(
  candidateName: string,
  potentialMatches: Array<{ name: string; score: number }>,
  model?: string
): Promise<{ results: Array<{ index: number; adjustedScore: number }>; tokenUsage?: TokenUsage }> {
  try {
    const prompt = `
Evaluate how well these company names match ${JSON.stringify(candidateName)}:

${potentialMatches.map((match, index) => `${index}. ${JSON.stringify(match.name)} (current score: ${match.score.toFixed(2)})`).join('\n')}

For each company, provide an adjusted confidence score (0.0-1.0) based on:
- Name similarity
- Common abbreviations or variations
- Likelihood they refer to the same entity

Return a JSON array of objects with 'index' and 'adjustedScore' for each company.`;

    const schema = z.array(
      z.object({
        index: z.number().int().min(0),
        adjustedScore: z.number().min(0).max(1),
      })
    );

    const batchSchema = z.object({ results: schema });
    const result = await generateObject({
      model: getModel(model),
      schema: batchSchema,
      prompt: prompt + '\n\nReturn a JSON object with a "results" array.',
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof batchSchema>;
    const usage = result.usage;

    // Add token usage if available
    const tokenUsage = convertUsage(usage);

    return { results: object.results, tokenUsage: tokenUsage };
  } catch (error) {
    console.error('Error in batchEvaluateMatches:', error);
    // Return original scores on error
    return {
      results: potentialMatches.map((match, index) => ({
        index,
        adjustedScore: match.score,
      })),
    };
  }
}
