/**
 * AIEnhancementService - Enhances company matching with AI-powered analysis
 *
 * This service uses OpenAI (via Vercel AI SDK) to improve match quality when:
 * - Elasticsearch results have medium confidence (70-95%)
 * - Multiple similar companies create ambiguity
 * - User provides context that could help disambiguation
 * - Checking if short names are valid abbreviations
 *
 * Key strategies:
 * 1. **High-confidence enhancement**: Validates top matches and checks abbreviations
 * 2. **Context-based disambiguation**: Uses industry/region hints to pick best match
 * 3. **Batch evaluation**: Adjusts confidence for multiple low-scoring candidates
 *
 * The service intelligently decides when to use AI based on:
 * - Confidence thresholds (avoids AI for very high/low confidence)
 * - Presence of ambiguous matches (similar scores)
 * - Availability of user context
 *
 * All AI calls are tracked for debugging and cost monitoring when debug mode is enabled.
 */
import { config } from '../config/environment';
import {
  batchEvaluateMatches,
  checkAbbreviationMatch,
  disambiguateMultipleMatches,
  evaluateCompanyMatch,
} from '../tools/aiToolDefinitions';
import { CompanyMatch, ElasticsearchCompany } from '../types';
import { DebugTracker } from '../utils/debugTracker';
import { isPotentialAbbreviation } from '../utils/normalization';

export class AIEnhancementService {
  private debugTracker: DebugTracker;
  private model?: string;
  public suggestedSearches: string[] = [];

  constructor(debugTracker: DebugTracker, model?: string) {
    this.debugTracker = debugTracker;
    this.model = model;
  }

  /**
   * Enhance matching results using AI when appropriate
   */
  async enhanceMatchResults(
    candidateName: string,
    elasticsearchResults: ElasticsearchCompany[],
    confidenceScores: Map<string, number>,
    userContext?: { industry?: string; region?: string; size?: string }
  ): Promise<CompanyMatch[]> {
    if (elasticsearchResults.length === 0) {
      return [];
    }

    const topConfidence = Math.max(...confidenceScores.values());
    const hasAmbiguity = this.hasAmbiguousMatches(Array.from(confidenceScores.values()));
    const hasContext = Boolean(userContext && Object.keys(userContext).length > 0);

    // Determine if AI enhancement is needed
    if (!this.shouldUseAI(topConfidence, hasAmbiguity, hasContext)) {
      return this.convertToMatches(elasticsearchResults, confidenceScores);
    }

    // Apply appropriate AI enhancement strategy
    if (topConfidence >= config.matching.aiEnhancementMinThreshold) {
      return this.enhanceTopMatches(
        candidateName,
        elasticsearchResults,
        confidenceScores,
        userContext
      );
    } else if (hasContext && elasticsearchResults.length > 1) {
      return this.disambiguateWithContext(
        candidateName,
        elasticsearchResults,
        confidenceScores,
        userContext
      );
    } else {
      return this.batchEnhanceMatches(candidateName, elasticsearchResults, confidenceScores);
    }
  }

  /**
   * Enhance top scoring matches with AI validation
   */
  private async enhanceTopMatches(
    candidateName: string,
    results: ElasticsearchCompany[],
    confidenceScores: Map<string, number>,
    userContext?: { industry?: string; region?: string; size?: string }
  ): Promise<CompanyMatch[]> {
    const topResults = results.slice(0, 3); // Focus on top 3
    const enhancedMatches: CompanyMatch[] = [];

    for (const company of topResults) {
      // Skip companies without display_name
      if (!company.display_name) {
        continue;
      }
      const baseConfidence = confidenceScores.get(company.id) || 0;

      // Check if it might be an abbreviation
      const isAbbreviation =
        company.display_name && isPotentialAbbreviation(candidateName, company.display_name);

      let aiResult;
      if (isAbbreviation && candidateName.length <= 5) {
        console.log(
          `[AI Enhancement] Detected potential abbreviation: "${candidateName}" for "${company.display_name}"`
        );
        // Use abbreviation-specific check
        const startTime = Date.now();
        const abbrevResult = await checkAbbreviationMatch(
          {
            abbreviation: candidateName,
            fullName: company.display_name || company.id,
            industryContext: userContext?.industry,
          },
          this.model
        );
        const duration = Date.now() - startTime;

        this.debugTracker.trackAICall('checkAbbreviationMatch', abbrevResult.tokenUsage, duration);

        if (abbrevResult.isValidAbbreviation) {
          aiResult = {
            confidence: abbrevResult.confidence,
            reasoning: `Recognized as ${abbrevResult.commonUsage ? 'common' : 'valid'} abbreviation`,
            matchType: 'abbreviation' as const,
          };

          // Track suggested company name for potential re-search
          if (abbrevResult.suggestedCompanyName && abbrevResult.confidence < 0.9) {
            this.suggestedSearches.push(abbrevResult.suggestedCompanyName);
          }
        }
      }

      // If not handled as abbreviation, use general evaluation
      if (!aiResult) {
        const startTime = Date.now();
        const evalResult = await evaluateCompanyMatch(
          {
            candidateName,
            potentialMatch: {
              name: company.display_name || company.id,
            },
            context: userContext
              ? {
                  userIndustryHint: userContext.industry,
                  geographicRegion: userContext.region,
                  sizeCategory: userContext.size,
                }
              : undefined,
          },
          this.model
        );
        const duration = Date.now() - startTime;

        this.debugTracker.trackAICall('evaluateCompanyMatch', evalResult.tokenUsage, duration);
        aiResult = evalResult;

        // Track suggested company name for potential re-search
        if (evalResult.suggestedCompanyName && evalResult.confidence < 0.9) {
          this.suggestedSearches.push(evalResult.suggestedCompanyName);
        }
      }

      // Combine base confidence with AI confidence
      const finalConfidence = baseConfidence * 0.4 + aiResult.confidence * 0.6;

      enhancedMatches.push({
        id: company.id,
        name: company.display_name || company.id,
        confidence: finalConfidence,
        matchType: aiResult.matchType === 'unlikely' ? 'fuzzy' : aiResult.matchType,
        reasoning: aiResult.reasoning,
      });
    }

    // Sort by final confidence
    return enhancedMatches.sort((a, b) => b.confidence - a.confidence);
  }

  /**
   * Use AI to disambiguate when user context is available
   */
  private async disambiguateWithContext(
    candidateName: string,
    results: ElasticsearchCompany[],
    confidenceScores: Map<string, number>,
    userContext?: { industry?: string; region?: string; size?: string }
  ): Promise<CompanyMatch[]> {
    const competingMatches = results
      .slice(0, 5)
      .filter(c => c.display_name)
      .map(company => ({
        name: company.display_name || company.id,
        confidenceScore: confidenceScores.get(company.id) || 0,
      }));

    const startTime = Date.now();
    const disambiguationResult = await disambiguateMultipleMatches(
      {
        candidateName,
        competingMatches,
        userContext,
      },
      this.model
    );
    const duration = Date.now() - startTime;

    this.debugTracker.trackAICall(
      'disambiguateMultipleMatches',
      disambiguationResult.tokenUsage,
      duration
    );

    const bestMatch = results[disambiguationResult.bestMatchIndex];
    const baseConfidence = confidenceScores.get(bestMatch.id) || 0;

    // Create enhanced match for the best result
    return [
      {
        id: bestMatch.id,
        name: bestMatch.display_name || bestMatch.id,
        confidence: baseConfidence * 0.3 + disambiguationResult.confidence * 0.7,
        matchType: 'semantic',
        reasoning: disambiguationResult.reasoning,
      },
    ];
  }

  /**
   * Batch enhance low-confidence matches
   */
  private async batchEnhanceMatches(
    candidateName: string,
    results: ElasticsearchCompany[],
    confidenceScores: Map<string, number>
  ): Promise<CompanyMatch[]> {
    const potentialMatches = results
      .slice(0, 10)
      .filter(c => c.display_name)
      .map(company => ({
        name: company.display_name || company.id,
        score: confidenceScores.get(company.id) || 0,
      }));

    const startTime = Date.now();
    const aiScoresResult = await batchEvaluateMatches(candidateName, potentialMatches, this.model);
    const duration = Date.now() - startTime;

    if (aiScoresResult.tokenUsage) {
      this.debugTracker.trackAICall('batchEvaluateMatches', aiScoresResult.tokenUsage, duration);
    }
    const aiScores = aiScoresResult.results;

    return aiScores
      .filter(({ adjustedScore }) => adjustedScore >= config.matching.lowConfidenceThreshold)
      .map(({ index, adjustedScore }) => {
        const company = results[index];
        return {
          id: company.id,
          name: company.display_name || company.id,
          confidence: adjustedScore,
          matchType: 'fuzzy' as const,
          reasoning: 'AI-adjusted confidence score',
        };
      })
      .sort((a, b) => b.confidence - a.confidence);
  }

  /**
   * Convert elasticsearch results to match format without AI enhancement
   */
  private convertToMatches(
    results: ElasticsearchCompany[],
    confidenceScores: Map<string, number>
  ): CompanyMatch[] {
    return results.map(company => ({
      id: company.id,
      name: company.display_name || company.id,
      confidence: confidenceScores.get(company.id) || 0,
      matchType: 'fuzzy' as const,
    }));
  }

  /**
   * Determine if AI enhancement should be used
   */
  private shouldUseAI(
    topConfidence: number,
    hasAmbiguity: boolean,
    hasUserContext: boolean
  ): boolean {
    // Never use AI for very high confidence matches
    if (topConfidence >= config.matching.highConfidenceThreshold) {
      return false;
    }

    // Use AI for uncertain range
    if (
      topConfidence >= config.matching.aiEnhancementMinThreshold &&
      topConfidence < config.matching.aiEnhancementMaxThreshold
    ) {
      return true;
    }

    // Use AI if there's ambiguity or user context that could help
    if (
      (hasAmbiguity || hasUserContext) &&
      topConfidence >= config.matching.lowConfidenceThreshold
    ) {
      return true;
    }

    return false;
  }

  /**
   * Check if multiple matches are ambiguous
   */
  private hasAmbiguousMatches(scores: number[]): boolean {
    if (scores.length < 2) {
      return false;
    }

    const sortedScores = scores.sort((a, b) => b - a);
    return sortedScores[0] - sortedScores[1] <= config.matching.ambiguityThreshold;
  }
}
