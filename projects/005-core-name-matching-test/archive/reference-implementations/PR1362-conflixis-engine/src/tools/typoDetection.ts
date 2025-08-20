/**
 * Typo Detection and Correction Tool
 *
 * This module provides advanced typo detection and correction capabilities:
 * - Phonetic matching (Soundex, Metaphone) for similar-sounding names
 * - Keyboard distance algorithms for adjacent key errors
 * - Common typo pattern detection and correction
 * - Confidence scoring for corrections
 */

/**
 * Soundex algorithm implementation
 * Converts strings to phonetic representation
 */
export function soundex(str: string): string {
  if (!str || str.length === 0) return '';

  const s = str.toUpperCase().replace(/[^A-Z]/g, '');
  if (s.length === 0) return '';

  const soundexMap: { [key: string]: string } = {
    B: '1',
    F: '1',
    P: '1',
    V: '1',
    C: '2',
    G: '2',
    J: '2',
    K: '2',
    Q: '2',
    S: '2',
    X: '2',
    Z: '2',
    D: '3',
    T: '3',
    L: '4',
    M: '5',
    N: '5',
    R: '6',
  };

  let code = s[0];
  let previousCode = soundexMap[s[0]] || '0';

  for (let i = 1; i < s.length && code.length < 4; i++) {
    const currentCode = soundexMap[s[i]] || '0';
    if (currentCode !== '0' && currentCode !== previousCode) {
      code += currentCode;
    }
    if (currentCode !== '0') {
      previousCode = currentCode;
    }
  }

  return code.padEnd(4, '0');
}

/**
 * Metaphone algorithm implementation
 * More accurate phonetic matching than Soundex
 */
export function metaphone(str: string): string {
  if (!str || str.length === 0) return '';

  const s = str.toUpperCase().replace(/[^A-Z]/g, '');
  if (s.length === 0) return '';

  let result = '';
  let i = 0;

  // Handle initial letter combinations
  if (
    s.startsWith('KN') ||
    s.startsWith('GN') ||
    s.startsWith('PN') ||
    s.startsWith('AE') ||
    s.startsWith('WR')
  ) {
    i = 1;
  }
  if (s.startsWith('X')) {
    result = 'S';
    i = 1;
  }
  if (s.startsWith('WH')) {
    result = 'W';
    i = 2;
  }

  while (i < s.length) {
    const c = s[i];
    const next = i + 1 < s.length ? s[i + 1] : '';
    const prev = i > 0 ? s[i - 1] : '';

    switch (c) {
      case 'A':
      case 'E':
      case 'I':
      case 'O':
      case 'U':
      case 'Y':
        if (i === 0) result += c;
        break;
      case 'B':
        if (!(i === s.length - 1 && prev === 'M')) result += 'B';
        break;
      case 'C':
        if (next === 'H') {
          result += 'X';
          i++;
        } else if (next === 'I' || next === 'E' || next === 'Y') {
          result += 'S';
        } else {
          result += 'K';
        }
        break;
      case 'D':
        if (next === 'G' && (s[i + 2] === 'E' || s[i + 2] === 'Y' || s[i + 2] === 'I')) {
          result += 'J';
          i += 2;
        } else {
          result += 'T';
        }
        break;
      case 'F':
      case 'J':
      case 'L':
      case 'M':
      case 'N':
      case 'R':
        result += c;
        break;
      case 'G':
        if (next === 'H' && i + 2 < s.length) {
          i++;
        } else if (next === 'N' && i === s.length - 2) {
          // Silent G at end
        } else if (next === 'I' || next === 'E' || next === 'Y') {
          result += 'J';
        } else {
          result += 'K';
        }
        break;
      case 'H':
        if (prev && /[AEIOU]/.test(prev) && (!next || !/[AEIOU]/.test(next))) {
          // Silent H
        } else if (prev && /[CGPST]/.test(prev)) {
          // Also silent
        } else {
          result += 'H';
        }
        break;
      case 'K':
        if (prev !== 'C') result += 'K';
        break;
      case 'P':
        result += next === 'H' ? 'F' : 'P';
        if (next === 'H') i++;
        break;
      case 'Q':
        result += 'K';
        break;
      case 'S':
        if (next === 'H') {
          result += 'X';
          i++;
        } else if (next === 'I' && (s[i + 2] === 'O' || s[i + 2] === 'A')) {
          result += 'X';
        } else {
          result += 'S';
        }
        break;
      case 'T':
        if (next === 'H') {
          result += '0';
          i++;
        } else if (next === 'I' && (s[i + 2] === 'O' || s[i + 2] === 'A')) {
          result += 'X';
        } else {
          result += 'T';
        }
        break;
      case 'V':
        result += 'F';
        break;
      case 'W':
        if (next && /[AEIOU]/.test(next)) result += 'W';
        break;
      case 'X':
        result += 'KS';
        break;
      case 'Z':
        result += 'S';
        break;
    }
    i++;
  }

  return result;
}

/**
 * Calculate keyboard distance between two characters
 */
const KEYBOARD_LAYOUT = ['qwertyuiop', 'asdfghjkl', 'zxcvbnm'];

export function keyboardDistance(char1: string, char2: string): number {
  const c1 = char1.toLowerCase();
  const c2 = char2.toLowerCase();

  if (c1 === c2) return 0;

  let pos1: { row: number; col: number } | null = null;
  let pos2: { row: number; col: number } | null = null;

  for (let row = 0; row < KEYBOARD_LAYOUT.length; row++) {
    const col1 = KEYBOARD_LAYOUT[row].indexOf(c1);
    const col2 = KEYBOARD_LAYOUT[row].indexOf(c2);

    if (col1 !== -1) pos1 = { row, col: col1 };
    if (col2 !== -1) pos2 = { row, col: col2 };
  }

  if (!pos1 || !pos2) return 3; // Max distance for non-keyboard chars

  // Manhattan distance
  return Math.abs(pos1.row - pos2.row) + Math.abs(pos1.col - pos2.col);
}

/**
 * Detect if a string might contain keyboard typos
 */
export function detectKeyboardTypos(input: string, candidate: string): number {
  if (input.length !== candidate.length) return 0;

  let typoScore = 0;
  let differences = 0;

  for (let i = 0; i < input.length; i++) {
    if (input[i] !== candidate[i]) {
      differences++;
      const distance = keyboardDistance(input[i], candidate[i]);
      if (distance === 1) {
        typoScore += 0.9; // Adjacent keys
      } else if (distance === 2) {
        typoScore += 0.6; // Near keys
      } else {
        typoScore += 0.2; // Far keys
      }
    }
  }

  if (differences === 0) return 1;
  if (differences > 2) return 0;

  return typoScore / differences;
}

/**
 * Common typo patterns
 */
export interface TypoPattern {
  pattern: RegExp;
  replacement: string;
  description: string;
}

export const COMMON_TYPO_PATTERNS: TypoPattern[] = [
  // Doubled letters - but don't apply to single-letter repetitions in short words (like MMM, AAA)
  // This prevents breaking valid abbreviations
  { pattern: /(\w)\1{3,}/g, replacement: '$1$1', description: 'Multiple repeated characters (4+)' },
  {
    pattern: /([a-z])\1{2,}/g,
    replacement: '$1$1',
    description: 'Multiple repeated lowercase characters',
  },
  { pattern: /ss{2,}/g, replacement: 'ss', description: 'Multiple s characters' },
  { pattern: /ll{2,}/g, replacement: 'll', description: 'Multiple l characters' },

  // Common transpositions
  { pattern: /teh/gi, replacement: 'the', description: 'Common "the" typo' },
  { pattern: /recieve/gi, replacement: 'receive', description: 'ie/ei confusion' },

  // Missing spaces
  { pattern: /([a-z])([A-Z])/g, replacement: '$1 $2', description: 'Missing space before capital' },

  // Extra spaces
  { pattern: /\s+/g, replacement: ' ', description: 'Multiple spaces' },

  // Common corporate typos
  { pattern: /corportation/gi, replacement: 'corporation', description: 'Corporation typo' },
  { pattern: /compnay/gi, replacement: 'company', description: 'Company typo' },
  { pattern: /gropu/gi, replacement: 'group', description: 'Group typo' },
];

/**
 * Apply common typo corrections
 */
export function correctCommonTypos(input: string): { corrected: string; corrections: string[] } {
  let corrected = input;
  const corrections: string[] = [];

  for (const { pattern, replacement, description } of COMMON_TYPO_PATTERNS) {
    const before = corrected;
    corrected = corrected.replace(pattern, replacement);
    if (before !== corrected) {
      corrections.push(description);
    }
  }

  return { corrected, corrections };
}

/**
 * Generate typo variations for a company name
 */
export function generateTypoVariations(companyName: string): string[] {
  const variations = new Set<string>();
  const words = companyName.split(/\s+/);

  // Single character substitutions with adjacent keys
  for (let i = 0; i < companyName.length; i++) {
    const char = companyName[i].toLowerCase();
    const row = KEYBOARD_LAYOUT.findIndex(r => r.includes(char));

    if (row !== -1) {
      const col = KEYBOARD_LAYOUT[row].indexOf(char);

      // Check adjacent keys
      const adjacentKeys: string[] = [];
      if (row > 0 && KEYBOARD_LAYOUT[row - 1][col])
        adjacentKeys.push(KEYBOARD_LAYOUT[row - 1][col]);
      if (row < KEYBOARD_LAYOUT.length - 1 && KEYBOARD_LAYOUT[row + 1][col])
        adjacentKeys.push(KEYBOARD_LAYOUT[row + 1][col]);
      if (col > 0) adjacentKeys.push(KEYBOARD_LAYOUT[row][col - 1]);
      if (col < KEYBOARD_LAYOUT[row].length - 1) adjacentKeys.push(KEYBOARD_LAYOUT[row][col + 1]);

      for (const key of adjacentKeys) {
        const variation = companyName.substring(0, i) + key + companyName.substring(i + 1);
        variations.add(variation);
      }
    }
  }

  // Transposition of adjacent characters
  for (let i = 0; i < companyName.length - 1; i++) {
    const variation =
      companyName.substring(0, i) +
      companyName[i + 1] +
      companyName[i] +
      companyName.substring(i + 2);
    variations.add(variation);
  }

  // Missing characters - but skip if it's likely an abbreviation (all caps, short)
  // Don't remove characters from abbreviations like IBM, MMM, etc.
  const isLikelyAbbreviation = companyName.length <= 5 && /^[A-Z]+$/.test(companyName.trim());
  if (!isLikelyAbbreviation) {
    for (let i = 0; i < companyName.length; i++) {
      const variation = companyName.substring(0, i) + companyName.substring(i + 1);
      variations.add(variation);
    }
  }

  // Doubled characters
  for (let i = 0; i < companyName.length; i++) {
    const variation =
      companyName.substring(0, i + 1) + companyName[i] + companyName.substring(i + 1);
    variations.add(variation);
  }

  // Word order variations (for multi-word names)
  if (words.length === 2) {
    variations.add(`${words[1]} ${words[0]}`);
  }

  return Array.from(variations).filter(v => v !== companyName);
}

/**
 * Calculate phonetic similarity between two strings
 */
export function phoneticSimilarity(str1: string, str2: string): number {
  const soundex1 = soundex(str1);
  const soundex2 = soundex(str2);
  const metaphone1 = metaphone(str1);
  const metaphone2 = metaphone(str2);

  let score = 0;
  if (soundex1 === soundex2) score += 0.5;
  if (metaphone1 === metaphone2) score += 0.5;

  // Partial metaphone match
  if (score < 1 && metaphone1 && metaphone2) {
    const shorter = metaphone1.length < metaphone2.length ? metaphone1 : metaphone2;
    const longer = metaphone1.length < metaphone2.length ? metaphone2 : metaphone1;
    if (longer.startsWith(shorter)) {
      score += 0.3;
    }
  }

  return Math.min(score, 1);
}

/**
 * Main typo detection interface
 */
export interface TypoDetectionResult {
  isLikelyTypo: boolean;
  confidence: number;
  suggestedCorrection?: string;
  detectionMethod: 'phonetic' | 'keyboard' | 'pattern' | 'none';
  details?: string;
}

/**
 * Detect and suggest corrections for potential typos
 */
export function detectTypo(input: string, candidates: string[]): TypoDetectionResult {
  // Skip typo detection for likely abbreviations (all caps, 2-5 characters)
  const isLikelyAbbreviation = input.length >= 2 && input.length <= 5 && /^[A-Z]+$/.test(input);

  // First, try pattern-based correction (unless it's an abbreviation)
  if (!isLikelyAbbreviation) {
    const { corrected, corrections } = correctCommonTypos(input);
    if (corrections.length > 0) {
      return {
        isLikelyTypo: true,
        confidence: 0.9,
        suggestedCorrection: corrected,
        detectionMethod: 'pattern',
        details: corrections.join(', '),
      };
    }
  }

  // Check against candidates
  let bestMatch: { candidate: string; score: number; method: 'phonetic' | 'keyboard' } | null =
    null;

  for (const candidate of candidates) {
    // Phonetic matching
    const phoneticScore = phoneticSimilarity(input, candidate);
    if (phoneticScore > 0.8 && (!bestMatch || phoneticScore > bestMatch.score)) {
      bestMatch = { candidate, score: phoneticScore, method: 'phonetic' };
    }

    // Keyboard distance matching
    const keyboardScore = detectKeyboardTypos(input, candidate);
    if (keyboardScore > 0.7 && (!bestMatch || keyboardScore > bestMatch.score)) {
      bestMatch = { candidate, score: keyboardScore, method: 'keyboard' };
    }
  }

  if (bestMatch && bestMatch.score > 0.7) {
    return {
      isLikelyTypo: true,
      confidence: bestMatch.score,
      suggestedCorrection: bestMatch.candidate,
      detectionMethod: bestMatch.method,
      details: `${bestMatch.method} match with score ${bestMatch.score.toFixed(2)}`,
    };
  }

  return {
    isLikelyTypo: false,
    confidence: 0,
    detectionMethod: 'none',
  };
}
