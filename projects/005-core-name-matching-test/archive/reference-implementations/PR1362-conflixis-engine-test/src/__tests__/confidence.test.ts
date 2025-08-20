import { ConfidenceFactors } from '../../types';
import {
  calculateContextMatch,
  calculateFinalConfidence,
  determineMatchType,
  hasAmbiguousMatches,
  shouldUseAI,
} from '../confidence';

describe('calculateFinalConfidence', () => {
  it('should calculate weighted average correctly', () => {
    const factors: ConfidenceFactors = {
      elasticsearchScore: 0.8,
      stringSimilarity: 0.9,
      aiConfidence: 0.95,
      contextMatch: 0.7,
    };

    const confidence = calculateFinalConfidence(factors);
    expect(confidence).toBeGreaterThan(0.8);
    expect(confidence).toBeLessThanOrEqual(1.0);
  });

  it('should handle missing AI confidence', () => {
    const factors: ConfidenceFactors = {
      elasticsearchScore: 0.8,
      stringSimilarity: 0.9,
      contextMatch: 0.7,
    };

    const confidence = calculateFinalConfidence(factors);
    expect(confidence).toBeGreaterThan(0.7);
    expect(confidence).toBeLessThanOrEqual(1.0);
  });

  it('should handle missing optional factors', () => {
    const factors: ConfidenceFactors = {
      elasticsearchScore: 0.8,
      stringSimilarity: 0.9,
    };

    const confidence = calculateFinalConfidence(factors);
    expect(confidence).toBeGreaterThan(0);
    expect(confidence).toBeLessThanOrEqual(1.0);
  });

  it('should clamp values between 0 and 1', () => {
    const factors: ConfidenceFactors = {
      elasticsearchScore: 1.5, // Invalid high value
      stringSimilarity: -0.5, // Invalid negative value
    };

    const confidence = calculateFinalConfidence(factors);
    expect(confidence).toBeGreaterThanOrEqual(0);
    expect(confidence).toBeLessThanOrEqual(1.0);
  });
});

describe('shouldUseAI', () => {
  it('should not use AI for high confidence matches', () => {
    expect(shouldUseAI(0.96, false, false)).toBe(false);
    expect(shouldUseAI(0.99, true, true)).toBe(false);
  });

  it('should use AI for uncertain range', () => {
    expect(shouldUseAI(0.75, false, false)).toBe(true);
    expect(shouldUseAI(0.85, false, false)).toBe(true);
  });

  it('should use AI when there is ambiguity', () => {
    expect(shouldUseAI(0.5, true, false)).toBe(true);
  });

  it('should use AI when user context is available', () => {
    expect(shouldUseAI(0.5, false, true)).toBe(true);
  });

  it('should not use AI for very low confidence', () => {
    expect(shouldUseAI(0.1, false, false)).toBe(false);
  });
});

describe('hasAmbiguousMatches', () => {
  it('should detect ambiguous matches', () => {
    expect(hasAmbiguousMatches([0.85, 0.82, 0.7])).toBe(true);
    expect(hasAmbiguousMatches([0.85, 0.84])).toBe(true);
  });

  it('should not flag clear winners', () => {
    expect(hasAmbiguousMatches([0.95, 0.7, 0.5])).toBe(false);
    expect(hasAmbiguousMatches([0.9, 0.6])).toBe(false);
  });

  it('should handle edge cases', () => {
    expect(hasAmbiguousMatches([0.9])).toBe(false);
    expect(hasAmbiguousMatches([])).toBe(false);
  });
});

describe('calculateContextMatch', () => {
  it('should match exact industry', () => {
    const company = { industry: 'Technology' };
    const context = { industry: 'Technology' };
    expect(calculateContextMatch(company, context)).toBe(1.0);
  });

  it('should partially match similar industries', () => {
    const company = { industry: 'Information Technology' };
    const context = { industry: 'Technology' };
    const score = calculateContextMatch(company, context);
    expect(score).toBe(0.5);
  });

  it('should handle domain/region matching', () => {
    const company = { domain: 'example.uk' };
    const context = { region: 'UK' };
    const score = calculateContextMatch(company, context);
    expect(score).toBe(0.5);
  });

  it('should return 0 for no context', () => {
    const company = { industry: 'Technology' };
    expect(calculateContextMatch(company)).toBe(0);
  });

  it('should handle missing fields', () => {
    const company = {};
    const context = { industry: 'Technology' };
    expect(calculateContextMatch(company, context)).toBe(0);
  });
});

describe('determineMatchType', () => {
  it('should identify exact matches', () => {
    expect(determineMatchType('Apple', 'Apple', 1.0)).toBe('exact');
    expect(determineMatchType('apple', 'APPLE', 1.0)).toBe('exact');
  });

  it('should identify abbreviations', () => {
    expect(determineMatchType('IBM', 'International Business Machines', 0.85, true)).toBe(
      'abbreviation'
    );
  });

  it('should identify variations', () => {
    expect(determineMatchType('Apple Inc', 'Apple', 0.92)).toBe('variation');
  });

  it('should identify semantic matches', () => {
    expect(determineMatchType('MS', 'Microsoft', 0.75)).toBe('semantic');
  });

  it('should default to fuzzy for low confidence', () => {
    expect(determineMatchType('ABC', 'XYZ Corp', 0.4)).toBe('fuzzy');
  });
});
