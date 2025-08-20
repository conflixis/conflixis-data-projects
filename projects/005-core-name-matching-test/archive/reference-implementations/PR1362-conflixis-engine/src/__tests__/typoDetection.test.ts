import {
  correctCommonTypos,
  detectKeyboardTypos,
  detectTypo,
  generateTypoVariations,
  keyboardDistance,
  metaphone,
  phoneticSimilarity,
  soundex,
} from '../typoDetection';

describe('Typo Detection', () => {
  describe('soundex', () => {
    it('should generate correct soundex codes', () => {
      expect(soundex('Johnson')).toBe('J525');
      expect(soundex('Jonson')).toBe('J525');
      expect(soundex('Microsoft')).toBe('M262');
      expect(soundex('Mikrosoft')).toBe('M262');
    });

    it('should handle empty strings', () => {
      expect(soundex('')).toBe('');
      expect(soundex('   ')).toBe('');
    });
  });

  describe('metaphone', () => {
    it('should generate correct metaphone codes', () => {
      expect(metaphone('Johnson')).toContain('J');
      expect(metaphone('Jonson')).toContain('J');
      expect(metaphone('Philip')).toBe('FLP');
      expect(metaphone('Phillip')).toBe('FLP');
    });
  });

  describe('keyboardDistance', () => {
    it('should calculate correct distances', () => {
      expect(keyboardDistance('a', 'a')).toBe(0);
      expect(keyboardDistance('a', 's')).toBe(1);
      expect(keyboardDistance('a', 'd')).toBe(2);
      expect(keyboardDistance('q', 'p')).toBe(9);
    });

    it('should handle non-keyboard characters', () => {
      expect(keyboardDistance('1', '2')).toBe(3);
      expect(keyboardDistance('!', '@')).toBe(3);
    });
  });

  describe('detectKeyboardTypos', () => {
    it('should detect adjacent key typos', () => {
      expect(detectKeyboardTypos('teh', 'the')).toBeGreaterThan(0.8);
      expect(detectKeyboardTypos('compnay', 'company')).toBeGreaterThan(0.7);
    });

    it('should return 0 for different length strings', () => {
      expect(detectKeyboardTypos('test', 'testing')).toBe(0);
    });

    it('should return 1 for identical strings', () => {
      expect(detectKeyboardTypos('company', 'company')).toBe(1);
    });
  });

  describe('correctCommonTypos', () => {
    it('should correct common typos', () => {
      expect(correctCommonTypos('teh company').corrected).toBe('the company');
      expect(correctCommonTypos('corportation').corrected).toBe('corporation');
      expect(correctCommonTypos('multiple   spaces').corrected).toBe('multiple spaces');
    });

    it('should track corrections made', () => {
      const result = correctCommonTypos('teh compnay');
      expect(result.corrections).toContain('Common "the" typo');
      expect(result.corrections).toContain('Company typo');
    });
  });

  describe('generateTypoVariations', () => {
    it('should generate keyboard variations', () => {
      const variations = generateTypoVariations('IBM');
      expect(variations).toContain('IBN');
      expect(variations).toContain('IVM');
      expect(variations).toContain('UBM');
    });

    it('should generate transposition variations', () => {
      const variations = generateTypoVariations('the');
      expect(variations).toContain('hte');
      expect(variations).toContain('teh');
    });

    it('should generate missing character variations', () => {
      const variations = generateTypoVariations('test');
      expect(variations).toContain('est');
      expect(variations).toContain('tst');
      expect(variations).toContain('tet');
    });
  });

  describe('phoneticSimilarity', () => {
    it('should detect phonetically similar words', () => {
      expect(phoneticSimilarity('Johnson', 'Jonson')).toBeGreaterThan(0.5);
      expect(phoneticSimilarity('Microsoft', 'Mikrosoft')).toBeGreaterThan(0.5);
      expect(phoneticSimilarity('Philip', 'Phillip')).toBeGreaterThan(0.5);
    });

    it('should return 0 for phonetically different words', () => {
      expect(phoneticSimilarity('Apple', 'Microsoft')).toBe(0);
      expect(phoneticSimilarity('Google', 'Amazon')).toBe(0);
    });
  });

  describe('detectTypo', () => {
    it('should detect pattern-based typos', () => {
      const result = detectTypo('teh company', []);
      expect(result.isLikelyTypo).toBe(true);
      expect(result.suggestedCorrection).toBe('the company');
      expect(result.detectionMethod).toBe('pattern');
    });

    it('should detect phonetic matches', () => {
      const result = detectTypo('Mikrosoft', ['Microsoft', 'Apple', 'Google']);
      expect(result.isLikelyTypo).toBe(true);
      expect(result.suggestedCorrection).toBe('Microsoft');
      expect(result.detectionMethod).toBe('phonetic');
    });

    it('should detect keyboard typos', () => {
      const result = detectTypo('Goofle', ['Google', 'Apple', 'Microsoft']);
      expect(result.isLikelyTypo).toBe(true);
      expect(result.confidence).toBeGreaterThan(0.7);
    });

    it('should not detect typo when no match found', () => {
      const result = detectTypo('ValidCompany', ['Apple', 'Google']);
      expect(result.isLikelyTypo).toBe(false);
      expect(result.detectionMethod).toBe('none');
    });
  });
});
