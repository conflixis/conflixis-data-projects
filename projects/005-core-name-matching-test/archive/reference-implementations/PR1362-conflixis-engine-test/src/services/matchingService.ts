/**
 * MatchingService - Core orchestrator for the company matching process
 *
 * This service coordinates the entire matching workflow:
 * 1. **Elasticsearch Search**: Queries the database for potential matches
 * 2. **Confidence Calculation**: Combines multiple signals to score each candidate:
 *    - Elasticsearch relevance score (normalized)
 *    - String similarity (multiple algorithms)
 *    - Context matching (industry, region, size)
 *    - Abbreviation detection
 * 3. **Fast Path**: Returns immediately for very high confidence matches (>95%)
 * 4. **AI Enhancement**: Applies AI analysis for medium confidence results
 * 5. **Response Formatting**: Returns appropriate response based on confidence levels
 *
 * Key confidence thresholds:
 * - >= 90%: Single high-confidence match
 * - 30-89%: Multiple potential matches
 * - < 30%: No match
 *
 * The service supports:
 * - Debug mode for tracking AI token usage and performance
 * - Context-aware matching using industry/region/size hints
 * - Multiple name fields (display_name, submitting_name, parent_display_name)
 * - Intelligent abbreviation detection and scoring
 */
import { compareTwoStrings } from 'string-similarity';
import { config } from '../config/environment';
import { detectCorporateStructure, identifyParentCompany } from '../tools/corporateStructure';
import { detectHistoricalCompany, detectHistoricalName } from '../tools/historicalNames';
import { generateSmartQueries, rewriteQuery } from '../tools/queryRewriting';
import { detectTypo, generateTypoVariations } from '../tools/typoDetection';
import {
  CompanyMatch,
  CompanyMatchRequest,
  CompanyMatchResponse,
  ConfidenceFactors,
  DebugInfo,
  ElasticsearchCompany,
} from '../types';
import {
  calculateContextMatch,
  calculateFinalConfidence,
  determineMatchType,
} from '../utils/confidence';
import { DebugTracker } from '../utils/debugTracker';
import {
  calculateCharacterSimilarity,
  isPotentialAbbreviation,
  normalizeCompanyName,
} from '../utils/normalization';
import { AIEnhancementService } from './aiEnhancementService';
import { ElasticsearchService } from './elasticsearchService';
import { ExactMatchCacheService } from './exactMatchCacheService';

export class MatchingService {
  private elasticsearchService = new ElasticsearchService();
  private cacheService = new ExactMatchCacheService(this.elasticsearchService);

  /**
   * Main entry point for company matching
   */
  async matchCompany(
    request: CompanyMatchRequest,
    searchDepth: number = 0,
    searchHistory: Set<string> = new Set()
  ): Promise<CompanyMatchResponse & { debug?: DebugInfo }> {
    const startTime = Date.now();
    const debugTracker = new DebugTracker(request.debug || false);
    const aiEnhancementService = new AIEnhancementService(debugTracker, request.model);

    // Check exact match cache first for fast path (unless skipCache is true)
    if (searchDepth === 0 && !request.skipCache) {
      const cacheResult = await this.cacheService.lookupCache(request.companyName, request.context);

      if (cacheResult.hit && cacheResult.company) {
        console.log(
          `[Matching] Cache hit for "${request.companyName}" → company: ${cacheResult.companyId}`
        );

        const cacheEntry = cacheResult.cacheEntry!;
        const response: CompanyMatchResponse & { debug?: DebugInfo } = {
          status: 'match',
          match: {
            id: cacheResult.company.id,
            name: cacheResult.company.display_name || cacheResult.company.id,
            confidence: cacheEntry.confidence,
            matchType:
              (cacheEntry.match_type as
                | 'exact'
                | 'abbreviation'
                | 'variation'
                | 'semantic'
                | 'fuzzy') || 'exact',
            reasoning: `Exact match found in cache (matched ${cacheEntry.match_count} times)`,
          },
        };

        if (debugTracker.isEnabled()) {
          debugTracker.trackESSearch('cache-lookup', 1, Date.now() - startTime);
          response.debug = debugTracker.getDebugInfo();
        }

        return response;
      }
    } else if (request.skipCache && searchDepth === 0) {
      console.log(`[Matching] Skipping cache lookup for "${request.companyName}" (skipCache=true)`);
    }

    // Track search history to avoid loops
    const normalizedSearchTerm = request.companyName.toLowerCase().trim();
    if (searchHistory.has(normalizedSearchTerm)) {
      console.log(`[Matching] Skipping duplicate search for: "${request.companyName}"`);
      return {
        status: 'no_match',
        message: 'Already searched this term',
      };
    }
    searchHistory.add(normalizedSearchTerm);

    if (searchDepth > 0) {
      console.log(`[Matching] Recursive search depth ${searchDepth} for: "${request.companyName}"`);
    }

    try {
      // Step 0: Apply query enhancements before searching
      let searchQuery = request.companyName;
      const enhancedQueries: string[] = [];

      // Check for typos and suggest corrections
      const typoDetection = detectTypo(searchQuery, []);
      if (typoDetection.isLikelyTypo && typoDetection.suggestedCorrection) {
        console.log(
          `[Matching] Detected typo: "${searchQuery}" → "${typoDetection.suggestedCorrection}"`
        );
        searchQuery = typoDetection.suggestedCorrection;
      }

      // Generate query variations
      const queryRewriting = rewriteQuery(searchQuery);
      if (queryRewriting.primaryVariation) {
        enhancedQueries.push(queryRewriting.primaryVariation);
      }
      enhancedQueries.push(...queryRewriting.variations.slice(0, 3));

      // Step 1: Search Elasticsearch
      const esStartTime = Date.now();
      let elasticsearchResults = await this.elasticsearchService.searchCompanies(
        searchQuery,
        20 // Get more results for better matching
      );
      const esDuration = Date.now() - esStartTime;

      // Track ES search in debug info
      debugTracker.trackESSearch(searchQuery, elasticsearchResults.length, esDuration);

      // Calculate initial confidence if we have results
      let topConfidence = 0;
      if (elasticsearchResults.length > 0) {
        const initialScores = this.calculateInitialConfidences(
          request.companyName,
          elasticsearchResults,
          request.context
        );
        topConfidence = Math.max(...initialScores.values());
      }

      // Try enhanced queries if no results OR low confidence results
      if ((elasticsearchResults.length === 0 || topConfidence < 0.9) && searchDepth === 0) {
        console.log(
          `[Matching] ${elasticsearchResults.length === 0 ? 'No matches' : `Low confidence (${topConfidence.toFixed(2)})`} for "${request.companyName}", trying enhancement strategies`
        );

        // Try enhanced queries if we have any
        for (const enhancedQuery of enhancedQueries) {
          if (enhancedQuery && enhancedQuery !== searchQuery) {
            console.log(`[Matching] Trying enhanced query: "${enhancedQuery}"`);
            const enhancedResults = await this.elasticsearchService.searchCompanies(
              enhancedQuery,
              10
            );
            if (enhancedResults.length > 0) {
              // Calculate confidence for enhanced results
              const enhancedScores = this.calculateInitialConfidences(
                request.companyName,
                enhancedResults,
                request.context
              );
              const enhancedTopScore = Math.max(...enhancedScores.values());

              // Use enhanced results if they have better confidence
              if (enhancedTopScore > topConfidence) {
                console.log(
                  `[Matching] Enhanced query "${enhancedQuery}" has better confidence (${enhancedTopScore.toFixed(2)} > ${topConfidence.toFixed(2)})`
                );
                elasticsearchResults = enhancedResults;
                searchQuery = enhancedQuery;
                topConfidence = enhancedTopScore;

                // If we found a high confidence match, stop searching
                if (enhancedTopScore >= 0.9) {
                  break;
                }
              }
            }
          }
        }

        // If still no results or low confidence, check for corporate structure patterns
        if (elasticsearchResults.length === 0 || topConfidence < 0.9) {
          const corporateStructure = detectCorporateStructure(request.companyName);
          if (
            corporateStructure.isSubsidiary &&
            corporateStructure.parentCompanySuggestions.length > 0
          ) {
            console.log(`[Matching] Detected subsidiary pattern, checking parent companies`);

            // Try parent company suggestions
            for (const parentSuggestion of corporateStructure.parentCompanySuggestions.slice(
              0,
              2
            )) {
              const parentResults = await this.elasticsearchService.searchCompanies(
                parentSuggestion,
                10
              );
              if (parentResults.length > 0) {
                console.log(`[Matching] Found results for parent company: "${parentSuggestion}"`);

                // Use AI to verify the subsidiary relationship
                const parentVerification = await identifyParentCompany(
                  {
                    companyName: request.companyName,
                    context: request.context,
                  },
                  request.model
                );

                if (parentVerification.tokenUsage) {
                  debugTracker.trackAICall(
                    'identifyParentCompany',
                    parentVerification.tokenUsage,
                    0
                  );
                }

                if (parentVerification.hasParent && parentVerification.confidence >= 0.7) {
                  const recursiveRequest = {
                    ...request,
                    companyName: parentVerification.parentCompanyName || parentSuggestion,
                  };

                  const recursiveResult = await this.matchCompany(
                    recursiveRequest,
                    searchDepth + 1,
                    searchHistory
                  );

                  if (recursiveResult.status === 'match' && recursiveResult.match) {
                    recursiveResult.match.reasoning = `Found parent company: "${request.companyName}" is ${parentVerification.relationshipType} of "${recursiveResult.match.name}". ${recursiveResult.match.reasoning || ''}`;
                  }

                  if (debugTracker.isEnabled()) {
                    return { ...recursiveResult, debug: debugTracker.getDebugInfo() };
                  }
                  return recursiveResult;
                }
              }
            }
          }

          // Check for historical names
          const historicalDetection = detectHistoricalName(request.companyName);
          if (
            historicalDetection.isLikelyHistorical &&
            historicalDetection.currentNameSuggestions.length > 0
          ) {
            console.log(`[Matching] Detected historical name pattern, checking current names`);

            // Use AI to get more specific historical information
            const historicalInfo = await detectHistoricalCompany(
              {
                companyName: request.companyName,
                context: request.context,
              },
              request.model
            );

            if (historicalInfo.tokenUsage) {
              debugTracker.trackAICall('detectHistoricalCompany', historicalInfo.tokenUsage, 0);
            }

            if (historicalInfo.isHistorical && historicalInfo.currentName) {
              const recursiveRequest = {
                ...request,
                companyName: historicalInfo.currentName,
              };

              const recursiveResult = await this.matchCompany(
                recursiveRequest,
                searchDepth + 1,
                searchHistory
              );

              if (recursiveResult.status === 'match' && recursiveResult.match) {
                recursiveResult.match.reasoning = `Historical name detected: "${request.companyName}" is now "${recursiveResult.match.name}" (${historicalInfo.historicalType}${historicalInfo.yearOfChange ? ` in ${historicalInfo.yearOfChange}` : ''}). ${recursiveResult.match.reasoning || ''}`;
              }

              if (debugTracker.isEnabled()) {
                return { ...recursiveResult, debug: debugTracker.getDebugInfo() };
              }
              return recursiveResult;
            }
          }
        }

        // If still no results or low confidence, try abbreviation guessing
        if (
          (elasticsearchResults.length === 0 || topConfidence < 0.9) &&
          request.companyName.length <= 10
        ) {
          console.log(
            `[Matching] No matches for "${request.companyName}", attempting to guess abbreviation`
          );

          const { guessCompanyFromAbbreviation } = await import('../tools/aiToolDefinitions');
          const guessStartTime = Date.now();
          const guessResult = await guessCompanyFromAbbreviation(
            {
              abbreviation: request.companyName,
              context: request.context,
            },
            request.model
          );
          const guessDuration = Date.now() - guessStartTime;

          if (guessResult.tokenUsage) {
            debugTracker.trackAICall(
              'guessCompanyFromAbbreviation',
              guessResult.tokenUsage,
              guessDuration
            );
          }

          // If we have high-confidence guesses, try searching for them
          const topGuess = guessResult.possibleCompanies?.[0];
          if (topGuess && topGuess.confidence >= 0.7) {
            console.log(
              `[Matching] AI guessed: "${topGuess.name}" (confidence: ${topGuess.confidence})`
            );

            const recursiveRequest = {
              ...request,
              companyName: topGuess.name,
            };

            const recursiveResult = await this.matchCompany(
              recursiveRequest,
              searchDepth + 1,
              searchHistory
            );

            // If the recursive search found a match, use it
            if (
              recursiveResult.status === 'match' ||
              (recursiveResult.status === 'potential_matches' &&
                recursiveResult.potentialMatches.some(m => m.confidence >= 0.8))
            ) {
              // Update reasoning to show it was found via abbreviation guess
              if (recursiveResult.status === 'match' && recursiveResult.match) {
                recursiveResult.match.reasoning = `Found via abbreviation guess: "${request.companyName}" → "${topGuess.name}" (${topGuess.reasoning}). ${recursiveResult.match.reasoning || ''}`;
              }

              // Add debug info if enabled
              if (debugTracker.isEnabled()) {
                return { ...recursiveResult, debug: debugTracker.getDebugInfo() };
              }
              return recursiveResult;
            }
          }
        }

        // If still no matches or low confidence, use AI to generate smart queries
        if (elasticsearchResults.length === 0 || topConfidence < 0.9) {
          console.log(`[Matching] Using AI to generate smart queries`);

          const smartQueriesResult = await generateSmartQueries(
            {
              companyName: request.companyName,
              context: {
                industry: request.context?.industry,
                previousFailures: [searchQuery, ...enhancedQueries],
              },
            },
            request.model
          );

          if (smartQueriesResult.tokenUsage) {
            debugTracker.trackAICall('generateSmartQueries', smartQueriesResult.tokenUsage, 0);
          }

          // Try the top AI-generated queries
          for (const smartQuery of smartQueriesResult.queries.slice(0, 2)) {
            if (smartQuery.confidence >= 0.6) {
              console.log(
                `[Matching] Trying AI-generated query: "${smartQuery.query}" (${smartQuery.type})`
              );

              const recursiveRequest = {
                ...request,
                companyName: smartQuery.query,
              };

              const recursiveResult = await this.matchCompany(
                recursiveRequest,
                searchDepth + 1,
                searchHistory
              );

              if (
                recursiveResult.status === 'match' ||
                (recursiveResult.status === 'potential_matches' &&
                  recursiveResult.potentialMatches.some(m => m.confidence >= 0.7))
              ) {
                if (recursiveResult.status === 'match' && recursiveResult.match) {
                  recursiveResult.match.reasoning = `Found via AI query rewriting: "${request.companyName}" → "${smartQuery.query}" (${smartQuery.reasoning}). ${recursiveResult.match.reasoning || ''}`;
                }

                if (debugTracker.isEnabled()) {
                  return { ...recursiveResult, debug: debugTracker.getDebugInfo() };
                }
                return recursiveResult;
              }
            }
          }
        }
      }

      if (elasticsearchResults.length === 0) {
        const response: CompanyMatchResponse & { debug?: DebugInfo } = {
          status: 'no_match',
          message: 'No matches found',
        };
        if (debugTracker.isEnabled()) {
          response.debug = debugTracker.getDebugInfo();
        }
        return response;
      }

      // Step 2: Calculate initial confidence scores
      const confidenceScores = this.calculateInitialConfidences(
        request.companyName,
        elasticsearchResults,
        request.context
      );

      // Step 3: Check if we need AI enhancement
      const topScore = Math.max(...confidenceScores.values());

      // Fast path for high confidence matches
      if (topScore >= config.matching.highConfidenceThreshold) {
        const topMatch = this.getTopMatch(elasticsearchResults, confidenceScores);
        const matchType = determineMatchType(
          request.companyName,
          topMatch.company.display_name || '',
          topMatch.confidence,
          topMatch.company.display_name
            ? isPotentialAbbreviation(request.companyName, topMatch.company.display_name)
            : false
        );

        // Update cache for high-confidence matches (unless skipCache is true)
        if (searchDepth === 0 && !request.skipCache) {
          this.updateCacheAsync(
            topMatch.company.id,
            request.companyName,
            topMatch.confidence,
            matchType,
            request.context
          );
        }

        const response: CompanyMatchResponse & { debug?: DebugInfo } = {
          status: 'match',
          match: {
            id: topMatch.company.id,
            name: topMatch.company.display_name || topMatch.company.id,
            // aliases: topMatch.company.aliases,
            // domain: topMatch.company.domain,
            // industry: topMatch.company.industry,
            confidence: topMatch.confidence,
            matchType,
          },
        };
        if (debugTracker.isEnabled()) {
          response.debug = debugTracker.getDebugInfo();
        }
        return response;
      }

      // Step 4: Apply AI enhancement if needed
      const enhancedMatches = await aiEnhancementService.enhanceMatchResults(
        request.companyName,
        elasticsearchResults,
        confidenceScores,
        request.context
      );

      // Step 5: Format response based on confidence
      let response = this.formatResponse(enhancedMatches);

      // Update cache for high-confidence matches (unless skipCache is true)
      if (
        searchDepth === 0 &&
        !request.skipCache &&
        response.status === 'match' &&
        response.match
      ) {
        this.updateCacheAsync(
          response.match.id,
          request.companyName,
          response.match.confidence,
          response.match.matchType,
          request.context
        );
      }

      // Step 6: Check if AI suggested additional searches and we haven't exceeded depth limit
      const maxSearchDepth = 2; // Prevent infinite loops
      if (
        searchDepth < maxSearchDepth &&
        aiEnhancementService.suggestedSearches.length > 0 &&
        (response.status === 'no_match' ||
          (response.status === 'potential_matches' &&
            response.potentialMatches.every(m => m.confidence < 0.7)))
      ) {
        console.log(
          `[Matching] AI suggested additional searches: ${aiEnhancementService.suggestedSearches.join(', ')}`
        );

        // Try the first suggested search
        const suggestedSearch = aiEnhancementService.suggestedSearches[0];
        const recursiveRequest = {
          ...request,
          companyName: suggestedSearch,
        };

        console.log(
          `[Matching] Performing recursive search for: "${suggestedSearch}" (depth: ${searchDepth + 1})`
        );
        const recursiveResult = await this.matchCompany(
          recursiveRequest,
          searchDepth + 1,
          searchHistory
        );

        // If the recursive search found a good match, use it
        if (
          recursiveResult.status === 'match' ||
          (recursiveResult.status === 'potential_matches' &&
            recursiveResult.potentialMatches.some(m => m.confidence >= 0.8))
        ) {
          response = recursiveResult;

          // Update the match reasoning to indicate it was found via suggestion
          if (response.status === 'match' && response.match) {
            response.match.reasoning = `Found via AI suggestion: "${request.companyName}" → "${suggestedSearch}". ${response.match.reasoning || ''}`;
          }
        }
      }

      // Add debug info if enabled
      if (debugTracker.isEnabled()) {
        return { ...response, debug: debugTracker.getDebugInfo() };
      }

      return response;
    } finally {
      const duration = Date.now() - startTime;
      console.log(`Company matching completed in ${duration}ms`);
    }
  }

  /**
   * Calculate initial confidence scores for all results
   */
  private calculateInitialConfidences(
    candidateName: string,
    results: ElasticsearchCompany[],
    context?: { industry?: string; region?: string; size?: string }
  ): Map<string, number> {
    const confidenceMap = new Map<string, number>();
    const normalizedCandidate = normalizeCompanyName(candidateName);

    // Pre-calculate typo variations for comparison
    const typoVariations = generateTypoVariations(candidateName);

    // Get max Elasticsearch score for normalization
    interface ElasticsearchCompanyWithScore extends ElasticsearchCompany {
      _score?: number;
    }
    const maxElasticScore = Math.max(
      ...results.map((r: ElasticsearchCompanyWithScore) => r._score || 0)
    );

    for (const company of results as ElasticsearchCompanyWithScore[]) {
      // Skip companies without display_name
      if (!company.display_name) {
        continue;
      }
      const factors: ConfidenceFactors = {
        elasticsearchScore: 0,
        stringSimilarity: 0,
        contextMatch: 0,
      };

      // Elasticsearch score (normalized)
      const elasticScore = company._score || 0;
      factors.elasticsearchScore = this.elasticsearchService.normalizeScore(
        elasticScore,
        maxElasticScore
      );

      // String similarity scores
      const similarities: number[] = [];

      if (company.display_name) {
        similarities.push(
          compareTwoStrings(normalizedCandidate, normalizeCompanyName(company.display_name)),
          calculateCharacterSimilarity(candidateName, company.display_name)
        );
      }

      // Check submitting_name if available
      if (company.submitting_name) {
        // Handle submitting_name as array of strings
        const submittingNames = Array.isArray(company.submitting_name)
          ? company.submitting_name
          : [company.submitting_name];

        for (const submittingName of submittingNames) {
          if (submittingName) {
            similarities.push(
              compareTwoStrings(normalizedCandidate, normalizeCompanyName(submittingName)),
              calculateCharacterSimilarity(candidateName, submittingName)
            );
          }
        }
      }

      // Check parent_display_name if available
      if (company.parent_display_name) {
        similarities.push(
          compareTwoStrings(normalizedCandidate, normalizeCompanyName(company.parent_display_name)),
          calculateCharacterSimilarity(candidateName, company.parent_display_name)
        );
      }

      // Check for abbreviation match
      if (company.display_name && isPotentialAbbreviation(candidateName, company.display_name)) {
        similarities.push(0.85); // Boost for potential abbreviation
      }

      // Check for typo match
      if (company.display_name) {
        const typoDetectionResult = detectTypo(candidateName, [company.display_name]);
        if (typoDetectionResult.isLikelyTypo && typoDetectionResult.confidence > 0.7) {
          similarities.push(0.8); // Boost for likely typo
        }

        // Also check if any typo variations match
        for (const variation of typoVariations) {
          if (variation.toLowerCase() === company.display_name.toLowerCase()) {
            similarities.push(0.9); // High boost for exact typo variation match
            break;
          }
        }
      }

      factors.stringSimilarity = similarities.length > 0 ? Math.max(...similarities) : 0;

      // Context matching
      if (context) {
        factors.contextMatch = calculateContextMatch(company, context);
      }

      // Calculate final confidence
      const confidence = calculateFinalConfidence(factors);
      confidenceMap.set(company.id, confidence);
    }

    return confidenceMap;
  }

  /**
   * Get the top matching company
   */
  private getTopMatch(
    companies: ElasticsearchCompany[],
    confidenceScores: Map<string, number>
  ): { company: ElasticsearchCompany; confidence: number } {
    let topMatch: ElasticsearchCompany | null = null;
    let topConfidence = 0;

    for (const company of companies) {
      const confidence = confidenceScores.get(company.id) || 0;
      if (confidence > topConfidence) {
        topConfidence = confidence;
        topMatch = company;
      }
    }

    if (!topMatch) {
      throw new Error('No top match found');
    }

    return { company: topMatch, confidence: topConfidence };
  }

  /**
   * Format the response based on confidence levels
   */
  private formatResponse(matches: CompanyMatch[]): CompanyMatchResponse {
    if (matches.length === 0) {
      return {
        status: 'no_match',
        message: 'No matches found',
      };
    }

    const topMatch = matches[0];

    // Single high-confidence match
    if (topMatch.confidence >= 0.9) {
      return {
        status: 'match',
        match: topMatch,
      };
    }

    // Multiple potential matches
    const potentialMatches = matches.filter(
      m => m.confidence >= config.matching.lowConfidenceThreshold
    );

    if (potentialMatches.length > 0) {
      return {
        status: 'potential_matches',
        potentialMatches: potentialMatches.slice(0, 5), // Limit to top 5
      };
    }

    // No good matches
    return {
      status: 'no_match',
      message: 'No matches found',
    };
  }

  /**
   * Update cache asynchronously (fire and forget)
   */
  private updateCacheAsync(
    companyId: string,
    query: string,
    confidence: number,
    matchType: string,
    context?: { industry?: string; region?: string; size?: string }
  ): void {
    // Fire and forget - don't await
    this.cacheService.updateCache(companyId, query, confidence, matchType, context).catch(error => {
      console.error('[Matching] Failed to update cache:', error);
    });
  }

  /**
   * Get cache statistics
   */
  getCacheStats() {
    return this.cacheService.getStats();
  }
}
