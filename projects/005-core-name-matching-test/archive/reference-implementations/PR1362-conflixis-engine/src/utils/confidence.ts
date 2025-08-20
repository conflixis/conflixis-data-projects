import { config } from '../config/environment';
import { ConfidenceFactors } from '../types';

/**
 * Calculate weighted average of confidence factors
 */
export function calculateFinalConfidence(factors: ConfidenceFactors): number {
  const weights = {
    elasticsearchScore: 0.3,
    stringSimilarity: 0.2,
    aiConfidence: 0.4,
    contextMatch: 0.1,
  };

  // Adjust weights if AI confidence is not available
  if (factors.aiConfidence === undefined) {
    weights.elasticsearchScore = 0.5;
    weights.stringSimilarity = 0.4;
    weights.contextMatch = 0.1;
    weights.aiConfidence = 0;
  }

  // Calculate weighted sum
  let weightedSum = 0;
  let totalWeight = 0;

  if (factors.elasticsearchScore !== undefined) {
    weightedSum += factors.elasticsearchScore * weights.elasticsearchScore;
    totalWeight += weights.elasticsearchScore;
  }

  if (factors.stringSimilarity !== undefined) {
    weightedSum += factors.stringSimilarity * weights.stringSimilarity;
    totalWeight += weights.stringSimilarity;
  }

  if (factors.aiConfidence !== undefined) {
    weightedSum += factors.aiConfidence * weights.aiConfidence;
    totalWeight += weights.aiConfidence;
  }

  if (factors.contextMatch !== undefined) {
    weightedSum += factors.contextMatch * weights.contextMatch;
    totalWeight += weights.contextMatch;
  }

  // Normalize to 0-1 range
  const confidence = totalWeight > 0 ? weightedSum / totalWeight : 0;
  return Math.max(0, Math.min(1, confidence));
}

/**
 * Determine if AI enhancement should be used based on confidence
 */
export function shouldUseAI(
  topConfidence: number,
  hasAmbiguity: boolean,
  hasUserContext: boolean
): boolean {
  const { aiEnhancementMinThreshold, aiEnhancementMaxThreshold } = config.matching;

  // Never use AI for very high confidence matches
  if (topConfidence >= aiEnhancementMaxThreshold) {
    return false;
  }

  // Use AI for uncertain range
  if (topConfidence >= aiEnhancementMinThreshold && topConfidence < aiEnhancementMaxThreshold) {
    return true;
  }

  // Use AI if there's ambiguity or user context that could help
  if ((hasAmbiguity || hasUserContext) && topConfidence >= config.matching.lowConfidenceThreshold) {
    return true;
  }

  return false;
}

/**
 * Check if multiple matches are ambiguous
 */
export function hasAmbiguousMatches(confidenceScores: number[]): boolean {
  if (confidenceScores.length < 2) {
    return false;
  }

  const topScore = confidenceScores[0];
  const secondScore = confidenceScores[1];

  return topScore - secondScore <= config.matching.ambiguityThreshold;
}

/**
 * Calculate context match score
 */
export function calculateContextMatch(
  company: { country?: string; state?: string },
  userContext?: { industry?: string; region?: string }
): number {
  if (!userContext) {
    return 0;
  }

  let score = 0;
  let factors = 0;

  // Region match (country/state)
  if (userContext.region && (company.country || company.state)) {
    const regionLower = userContext.region.toLowerCase();

    if (company.country && company.country.toLowerCase() === regionLower) {
      score += 1.0;
      factors++;
    } else if (company.state && company.state.toLowerCase() === regionLower) {
      score += 0.8;
      factors++;
    } else if (company.country && regionLower.includes(company.country.toLowerCase())) {
      score += 0.5;
      factors++;
    } else if (company.state && regionLower.includes(company.state.toLowerCase())) {
      score += 0.4;
      factors++;
    }
  }

  return factors > 0 ? score / factors : 0;
}

/**
 * Determine match type based on various factors
 */
export function determineMatchType(
  candidateName: string,
  matchedName: string,
  confidence: number,
  isAbbreviation: boolean = false
): 'exact' | 'abbreviation' | 'variation' | 'semantic' | 'fuzzy' {
  const candidateLower = candidateName.toLowerCase().trim();
  const matchedLower = matchedName.toLowerCase().trim();

  if (candidateLower === matchedLower) {
    return 'exact';
  }

  if (isAbbreviation) {
    return 'abbreviation';
  }

  if (confidence >= 0.9) {
    return 'variation';
  }

  if (confidence >= 0.7) {
    return 'semantic';
  }

  return 'fuzzy';
}
