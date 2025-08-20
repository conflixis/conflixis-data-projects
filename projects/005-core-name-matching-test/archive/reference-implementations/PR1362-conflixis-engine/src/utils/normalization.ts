/**
 * Company name normalization utilities
 */

// Common company suffixes and their variations
const COMPANY_SUFFIXES = [
  'inc',
  'incorporated',
  'corp',
  'corporation',
  'ltd',
  'limited',
  'llc',
  'limited liability company',
  'plc',
  'public limited company',
  'co',
  'company',
  'intl',
  'international',
  'grp',
  'group',
  'assoc',
  'associates',
  'association',
  'partners',
  'partnership',
  'holdings',
  'holding',
  'ventures',
  'venture',
  'systems',
  'system',
  'technologies',
  'technology',
  'tech',
  'solutions',
  'solution',
  'services',
  'service',
  'industries',
  'industry',
  'enterprises',
  'enterprise',
];

// Common prefixes to handle
const COMMON_PREFIXES = ['the', 'a', 'an'];

/**
 * Normalize a company name for matching
 */
export function normalizeCompanyName(name: string): string {
  if (!name) return '';

  let normalized = name.toLowerCase().trim();

  // Remove common punctuation but keep important ones
  normalized = normalized
    .replace(/['"]/g, '') // Remove quotes
    .replace(/\s+/g, ' ') // Normalize whitespace
    .replace(/\./g, '') // Remove periods
    .replace(/,/g, '') // Remove commas
    .trim();

  // Remove common prefixes
  for (const prefix of COMMON_PREFIXES) {
    const pattern = new RegExp(`^${prefix}\\s+`, 'i');
    normalized = normalized.replace(pattern, '');
  }

  // Normalize company suffixes
  const suffixPattern = new RegExp(`\\s+(${COMPANY_SUFFIXES.join('|')})$`, 'i');
  normalized = normalized.replace(suffixPattern, '');

  // Handle special characters
  // Only replace & with 'and' if it's not part of a short abbreviation pattern
  if (name.length > 5 || !name.match(/^[A-Z&]+$/i)) {
    normalized = normalized.replace(/&/g, 'and');
  }
  normalized = normalized.replace(/\+/g, 'plus').replace(/@/g, 'at');

  // Remove extra whitespace
  normalized = normalized.replace(/\s+/g, ' ').trim();

  return normalized;
}

/**
 * Extract potential abbreviations from a company name
 */
export function extractAbbreviation(name: string): string {
  const words = name
    .split(/\s+/)
    .filter(word => word.length > 0 && !COMPANY_SUFFIXES.includes(word.toLowerCase()));

  // Take first letter of each significant word
  return words
    .map(word => word[0])
    .join('')
    .toUpperCase();
}

/**
 * Check if one name might be an abbreviation of another
 */
export function isPotentialAbbreviation(abbr: string, fullName: string): boolean {
  // Check for special cases like J&J, P&G, etc.
  if (abbr.includes('&') && abbr.length <= 5) {
    // Extract letters from abbreviation, treating & as a separator
    const abbrParts = abbr
      .toUpperCase()
      .split('&')
      .map(part => part.trim());
    const fullNameWords = fullName.toUpperCase().split(/\s+/);

    // Check if each part of the abbreviation matches the start of words in full name
    if (abbrParts.every(part => fullNameWords.some(word => word.startsWith(part)))) {
      return true;
    }
  }

  const normalizedAbbr = abbr.toUpperCase().replace(/[^A-Z]/g, '');
  const extractedAbbr = extractAbbreviation(fullName);

  // Direct match
  if (normalizedAbbr === extractedAbbr) {
    return true;
  }

  // Check if abbreviation letters appear in order in full name
  const fullNameLetters = fullName.toUpperCase().replace(/[^A-Z]/g, '');
  let abbrIndex = 0;

  for (const letter of fullNameLetters) {
    if (letter === normalizedAbbr[abbrIndex]) {
      abbrIndex++;
      if (abbrIndex === normalizedAbbr.length) {
        return true;
      }
    }
  }

  return false;
}

/**
 * Calculate character-level similarity between two strings
 */
export function calculateCharacterSimilarity(str1: string, str2: string): number {
  const s1 = str1.toLowerCase();
  const s2 = str2.toLowerCase();

  if (s1 === s2) return 1.0;

  const longer = s1.length > s2.length ? s1 : s2;
  const shorter = s1.length > s2.length ? s2 : s1;

  if (longer.length === 0) return 1.0;

  const editDistance = calculateLevenshteinDistance(longer, shorter);
  return (longer.length - editDistance) / longer.length;
}

/**
 * Calculate Levenshtein distance between two strings
 */
function calculateLevenshteinDistance(str1: string, str2: string): number {
  const matrix: number[][] = [];

  for (let i = 0; i <= str2.length; i++) {
    matrix[i] = [i];
  }

  for (let j = 0; j <= str1.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= str2.length; i++) {
    for (let j = 1; j <= str1.length; j++) {
      if (str2.charAt(i - 1) === str1.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1, // substitution
          matrix[i][j - 1] + 1, // insertion
          matrix[i - 1][j] + 1 // deletion
        );
      }
    }
  }

  return matrix[str2.length][str1.length];
}

/**
 * Extract key terms from a company name for semantic matching
 */
export function extractKeyTerms(name: string): string[] {
  const normalized = normalizeCompanyName(name);
  const words = normalized.split(/\s+/);

  // Filter out very common words and suffixes
  const stopWords = new Set([
    ...COMMON_PREFIXES,
    ...COMPANY_SUFFIXES,
    'of',
    'for',
    'by',
    'with',
    'in',
    'on',
    'at',
    'to',
    'from',
  ]);

  return words.filter(word => word.length > 2 && !stopWords.has(word.toLowerCase()));
}
