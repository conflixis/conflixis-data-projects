import {
  calculateCharacterSimilarity,
  extractAbbreviation,
  extractKeyTerms,
  isPotentialAbbreviation,
  normalizeCompanyName,
} from '../normalization';

describe('normalizeCompanyName', () => {
  it('should normalize basic company names', () => {
    expect(normalizeCompanyName('Apple Inc.')).toBe('apple');
    expect(normalizeCompanyName('Microsoft Corporation')).toBe('microsoft');
    expect(normalizeCompanyName('Google LLC')).toBe('google');
  });

  it('should handle special characters', () => {
    expect(normalizeCompanyName('AT&T')).toBe('atandt');
    expect(normalizeCompanyName('Procter & Gamble')).toBe('procter and gamble');
    expect(normalizeCompanyName('Yahoo!')).toBe('yahoo');
  });

  it('should remove common prefixes', () => {
    expect(normalizeCompanyName('The Walt Disney Company')).toBe('walt disney');
    expect(normalizeCompanyName('A Better Company')).toBe('better');
  });

  it('should handle multiple spaces', () => {
    expect(normalizeCompanyName('Space   Between   Words')).toBe('space between words');
  });
});

describe('extractAbbreviation', () => {
  it('should extract abbreviations correctly', () => {
    expect(extractAbbreviation('International Business Machines')).toBe('IBM');
    expect(extractAbbreviation('General Electric')).toBe('GE');
    expect(extractAbbreviation('Procter & Gamble')).toBe('PG');
  });

  it('should ignore common suffixes', () => {
    expect(extractAbbreviation('Apple Inc')).toBe('A');
    expect(extractAbbreviation('Microsoft Corporation')).toBe('M');
  });
});

describe('isPotentialAbbreviation', () => {
  it('should identify valid abbreviations', () => {
    expect(isPotentialAbbreviation('IBM', 'International Business Machines')).toBe(true);
    expect(isPotentialAbbreviation('GE', 'General Electric')).toBe(true);
    expect(isPotentialAbbreviation('MS', 'Microsoft')).toBe(true);
  });

  it('should reject invalid abbreviations', () => {
    expect(isPotentialAbbreviation('ABC', 'Microsoft')).toBe(false);
    expect(isPotentialAbbreviation('XYZ', 'Apple Inc')).toBe(false);
  });

  it('should handle case insensitivity', () => {
    expect(isPotentialAbbreviation('ibm', 'International Business Machines')).toBe(true);
    expect(isPotentialAbbreviation('Ibm', 'International Business Machines')).toBe(true);
  });
});

describe('calculateCharacterSimilarity', () => {
  it('should return 1.0 for identical strings', () => {
    expect(calculateCharacterSimilarity('Apple', 'Apple')).toBe(1.0);
    expect(calculateCharacterSimilarity('apple', 'APPLE')).toBe(1.0);
  });

  it('should calculate similarity correctly', () => {
    const similarity1 = calculateCharacterSimilarity('Apple', 'Aple');
    expect(similarity1).toBeGreaterThan(0.7);
    expect(similarity1).toBeLessThan(1.0);

    const similarity2 = calculateCharacterSimilarity('Microsoft', 'Microsofy');
    expect(similarity2).toBeGreaterThan(0.8);
    expect(similarity2).toBeLessThan(1.0);
  });

  it('should handle empty strings', () => {
    expect(calculateCharacterSimilarity('', '')).toBe(1.0);
    expect(calculateCharacterSimilarity('Apple', '')).toBeLessThan(0.5);
  });
});

describe('extractKeyTerms', () => {
  it('should extract meaningful terms', () => {
    const terms = extractKeyTerms('Apple Computer Inc');
    expect(terms).toEqual(['apple', 'computer']);
  });

  it('should filter out stop words and suffixes', () => {
    const terms = extractKeyTerms('The Bank of America Corporation');
    expect(terms).toEqual(['bank', 'america']);
  });

  it('should handle short words', () => {
    const terms = extractKeyTerms('3M Company');
    expect(terms).toEqual([]); // '3M' is too short, 'Company' is filtered
  });
});
