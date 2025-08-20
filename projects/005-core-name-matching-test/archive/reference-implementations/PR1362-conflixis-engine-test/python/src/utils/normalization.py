"""
Name normalization utilities
Ported from normalization.ts
"""

import re
import unicodedata
from typing import List, Tuple
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    HEALTHCARE_ABBREVIATIONS,
    HEALTHCARE_SYSTEM_PATTERNS,
    CORPORATE_SUFFIXES
)


def normalize_company_name(name: str) -> str:
    """
    Normalize a company name for matching
    
    Steps:
    1. Convert to lowercase
    2. Remove accents/diacritics
    3. Expand common abbreviations
    4. Remove punctuation
    5. Remove extra whitespace
    6. Remove corporate suffixes
    7. Apply healthcare-specific patterns
    """
    if not name:
        return ""
    
    # Convert to lowercase
    normalized = name.lower()
    
    # Remove accents and diacritics
    normalized = ''.join(
        c for c in unicodedata.normalize('NFD', normalized)
        if unicodedata.category(c) != 'Mn'
    )
    
    # Expand healthcare abbreviations
    for abbrev, full in HEALTHCARE_ABBREVIATIONS.items():
        # Match abbreviation with word boundaries
        pattern = r'\b' + re.escape(abbrev) + r'\b'
        normalized = re.sub(pattern, full, normalized)
    
    # Remove punctuation except spaces and hyphens (which might be meaningful)
    normalized = re.sub(r'[^\w\s-]', ' ', normalized)
    
    # Remove corporate suffixes
    for suffix in CORPORATE_SUFFIXES:
        pattern = r'\b' + re.escape(suffix) + r'\b'
        normalized = re.sub(pattern, '', normalized)
    
    # Apply healthcare system patterns
    for pattern, replacement in HEALTHCARE_SYSTEM_PATTERNS:
        normalized = re.sub(pattern, replacement, normalized)
    
    # Remove extra whitespace
    normalized = ' '.join(normalized.split())
    
    return normalized.strip()


def tokenize_name(name: str) -> List[str]:
    """
    Split a name into tokens for comparison
    """
    normalized = normalize_company_name(name)
    # Split on spaces and hyphens
    tokens = re.split(r'[\s-]+', normalized)
    # Remove empty tokens
    return [t for t in tokens if t]


def get_name_tokens_sorted(name: str) -> str:
    """
    Get tokens sorted alphabetically (for order-independent comparison)
    """
    tokens = tokenize_name(name)
    return ' '.join(sorted(tokens))


def is_potential_abbreviation(short_name: str, full_name: str) -> bool:
    """
    Check if short_name could be an abbreviation of full_name
    
    Examples:
    - 'KFH' could be 'Kaiser Foundation Hospital'
    - 'UCLA' could be 'University of California Los Angeles'
    """
    # Normalize both names
    short_normalized = normalize_company_name(short_name)
    full_normalized = normalize_company_name(full_name)
    
    # Remove spaces from short name (for acronyms)
    short_letters = re.sub(r'\s+', '', short_normalized)
    
    # If short name is too long, probably not an abbreviation
    if len(short_letters) > 10:
        return False
    
    # Get first letters of each word in full name
    full_tokens = tokenize_name(full_normalized)
    full_initials = ''.join(token[0] for token in full_tokens if token)
    
    # Check if short name matches initials
    if short_letters == full_initials:
        return True
    
    # Check if short name is contained in initials (partial abbreviation)
    if len(short_letters) >= 2 and short_letters in full_initials:
        return True
    
    # Check if removing common words helps
    skip_words = {'of', 'the', 'and', 'for', 'in', 'at', 'by'}
    filtered_tokens = [t for t in full_tokens if t not in skip_words]
    filtered_initials = ''.join(token[0] for token in filtered_tokens if token)
    
    if short_letters == filtered_initials:
        return True
    
    return False


def calculate_character_similarity(name1: str, name2: str) -> float:
    """
    Calculate character-level similarity between two names
    Uses Levenshtein distance ratio
    """
    if not name1 or not name2:
        return 0.0
    
    # Normalize names
    norm1 = normalize_company_name(name1)
    norm2 = normalize_company_name(name2)
    
    if norm1 == norm2:
        return 1.0
    
    # Calculate Levenshtein distance
    distance = levenshtein_distance(norm1, norm2)
    max_len = max(len(norm1), len(norm2))
    
    if max_len == 0:
        return 1.0
    
    # Convert distance to similarity (0 to 1)
    similarity = 1 - (distance / max_len)
    return max(0.0, min(1.0, similarity))


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein distance between two strings
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # j+1 instead of j since previous_row and current_row are one character longer than s2
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def extract_core_name(name: str) -> str:
    """
    Extract the core name, removing location and department info
    
    Examples:
    - 'Kaiser Permanente - Northern California' -> 'Kaiser Permanente'
    - 'Mayo Clinic Rochester' -> 'Mayo Clinic'
    - 'Cleveland Clinic - Heart Center' -> 'Cleveland Clinic'
    """
    normalized = normalize_company_name(name)
    
    # Remove content after common separators
    separators = [' - ', ' – ', ' — ', ', ', ' at ', ' of ']
    for sep in separators:
        if sep in normalized:
            normalized = normalized.split(sep)[0]
    
    # Remove common location indicators
    location_patterns = [
        r'\b(north|south|east|west|central|downtown|uptown|midtown)\b',
        r'\b(campus|location|branch|division|department|dept|center|unit)\b',
        r'\b\d+\b',  # Remove numbers (often addresses or campuses)
    ]
    
    for pattern in location_patterns:
        normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    normalized = ' '.join(normalized.split())
    
    return normalized.strip()


def get_name_variants(name: str) -> List[str]:
    """
    Generate common variants of a name for matching
    """
    variants = []
    
    # Original name
    variants.append(name)
    
    # Normalized
    normalized = normalize_company_name(name)
    variants.append(normalized)
    
    # Core name
    core = extract_core_name(name)
    if core != normalized:
        variants.append(core)
    
    # Sorted tokens
    sorted_tokens = get_name_tokens_sorted(name)
    if sorted_tokens != normalized:
        variants.append(sorted_tokens)
    
    # Without corporate suffixes
    no_suffix = remove_corporate_suffixes(name)
    if no_suffix != name:
        variants.append(normalize_company_name(no_suffix))
    
    # Apply healthcare patterns
    for pattern, replacement in HEALTHCARE_SYSTEM_PATTERNS:
        modified = re.sub(pattern, replacement, normalized)
        if modified != normalized:
            variants.append(modified)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_variants = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            unique_variants.append(v)
    
    return unique_variants


def remove_corporate_suffixes(name: str) -> str:
    """
    Remove corporate suffixes from a name
    """
    result = name
    for suffix in CORPORATE_SUFFIXES:
        pattern = r'\b' + re.escape(suffix) + r'\.?\b'
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    
    return ' '.join(result.split()).strip()